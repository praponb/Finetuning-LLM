from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_warehouse_id_operator_by_uni = Window.partitionBy("warehouse_id", "operator").orderBy("unit_price")

valid_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
    )
)

windowed = (
    valid_moves
    .withColumn("warehouse_id_avg_unit_price", F.round(F.avg("unit_price").over(w_warehouse_id), 2))
    .withColumn("unit_price_rank", F.rank().over(w_warehouse_id_operator_by_uni))
)

screened = (
    windowed.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id").isNotNull()) & (F.col("st.unit_price").between(58.6, 399.6)), "left_anti")
    .filter(F.col("unit_price_rank") <= 9)
)

qualified = (
    screened.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.month(F.col("st.moved_at")).isin(1, 2)), "left_anti")
    .filter(F.col("warehouse_id") <= 4)
)

top_per_group = (
    qualified
    .filter(F.col("unit_price_rank") <= 3)
    .drop("unit_price_rank")
)

derived = (
    top_per_group
    .filter(F.col("sku_id") >= 7)
    .select("*", F.initcap(F.trim(F.col("operator"))).alias("operator_clean"))
)

scored = (
    derived
    .filter(F.col("warehouse_id") >= 4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "warehouse_id_avg_unit_price",
        "operator_clean",
        F.length(F.trim(F.col("operator_clean"))).alias("operator_clean_len"),
    )
)

result = (
    scored
    .select(
        "id",
        "sku_id",
        "moved_at",
        "warehouse_id_avg_unit_price",
        "operator_clean",
        "operator_clean_len",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
    .orderBy("operator_clean_len")
)
