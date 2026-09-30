from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("city").orderBy("capacity", "id")

filtered = (
    df_warehouses
    .filter((F.lower(F.col("wh_code")).like("%l1%")) & (F.col("opened_on").isNotNull()))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        F.concat(F.col("city"), F.lit("#"), F.col("capacity").cast("string")).alias("city_ref"),
        F.substring(F.trim(F.col("wh_code")), 1, 3).alias("wh_code_prefix"),
    )
)

with_activity = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.hour(F.col("st.moved_at")).between(8, 10)), "left_anti")
)

deduped = (
    with_activity
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = (
    deduped
    .select(
        "*",
        F.length(F.trim(F.col("wh_code"))).alias("wh_code_len"),
        F.concat_ws("-", F.col("city_ref"), F.col("wh_code")).alias("city_ref_wh_code_label"),
        F.concat_ws(" / ", F.col("city"), F.col("city_ref")).alias("city_city_ref_label"),
    )
)

with_children = (
    scored.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.moved_at") < F.lit("2025-02-12 14:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.wh_code",
        "s.capacity",
        "s.city_ref",
        "s.wh_code_prefix",
        "s.wh_code_len",
        "s.city_city_ref_label",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

joined = (
    with_children.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("st.warehouse_id"), "inner")
    .filter(F.col("st.qty") < 326)
    .select(
        "s.*",
        F.col("st.qty").alias("move_qty"),
        F.col("st.unit_price").alias("move_unit_price_2"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

result = (
    joined
    .select(
        "id",
        "move_id",
        "move_unit_price_2",
        "move_unit_price",
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["move_unit_price_2", "move_unit_price"],
        variableColumnName="field",
        valueColumnName="reading",
    )
)
