from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("region").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-08-14"))) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.concat("store_name", F.lit(" "), "region").alias("store_name_region"),
        F.trim("city").alias("clean_city"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_filled"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.opened_date",
        "s.store_name_region",
        "s.clean_city",
        "s.region_filled",
        F.col("c.last_restock"),
        F.col("c.quantity"),
    )
)

latest_match = (
    with_last
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units").alias("store_sale_units"))
)

metrics = (
    latest_match
    .filter(F.col("region") == "West")
    .select(
        "id",
        "opened_date",
        "store_name_region",
        "clean_city",
        F.count("quantity").over(w_ordered).alias("last3_count_quantity"),
        F.sum("quantity").over(w_ordered).alias("last3_sum_quantity"),
        F.max("quantity").over(w_ordered).alias("last3_max_quantity"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.shipping_fee").isNotNull()), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.quantity") != 76), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        F.col("last3_sum_quantity").alias("LAST3_SUM_QUANTITY"),
        "last3_max_quantity",
    )
    .unpivot(
        ids=["id"],
        values=["LAST3_SUM_QUANTITY", "last3_max_quantity"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
