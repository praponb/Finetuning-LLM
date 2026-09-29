from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city").orderBy("revenue", "id")
w4 = Window.partitionBy("city").orderBy(F.col("revenue").desc())
w_ordered = Window.partitionBy("region").orderBy(F.col("revenue").desc_nulls_first())
w6 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

valid = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-07-21").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.concat("city", F.lit(", "), "region").alias("city_region"),
        F.concat("city", F.lit(", "), "region").alias("city_region_2"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

kept = (
    latest
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.product_id").isin([2, 7, 8, 10])), "left_anti")
)

with_last = (
    kept
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.*",
        F.col("c.revenue"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

enriched = (
    with_last
    .withColumn("prev_revenue", F.lag("revenue", 1).over(w3))
    .withColumn("revenue_percent_rank", F.percent_rank().over(w4))
    .withColumn("revenue_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w6))
)

latest_match = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn_2", F.row_number().over(w7))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date_2"))
)

filtered = (
    latest_match
    .filter(F.col("cumulative_sqft") == 32161)
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "city_region",
        "resolved_region",
        "rn",
        "revenue",
        "store_sale_sale_date",
        "prev_revenue",
        "revenue_percent_rank",
        "revenue_dense_rank",
        "cumulative_sqft",
        F.least("rn", "cumulative_sqft").alias("least_rn_cumulative_sqft"),
    )
)

result = (
    filtered
    .select(
        "id",
        "city",
        "opened_date",
        "resolved_region",
        "rn",
        "prev_revenue",
        "revenue_percent_rank",
        "revenue_dense_rank",
        "cumulative_sqft",
    )
    .orderBy(F.col("prev_revenue").desc_nulls_last(), "revenue_dense_rank")
)
