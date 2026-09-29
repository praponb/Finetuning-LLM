from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city").orderBy("store_sale_revenue", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("city").orderBy("inventory_quantity", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("MONTH", "opened_date"), "city").orderBy(F.col("sqft").desc(), "id")

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-03-06").cast("date")) & F.col("region").isNotNull())
    .withColumn("clean_store_name", F.trim("store_name"))
    .withColumn("region_title", F.initcap(F.trim("region")))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.opened_date",
        "s.sqft",
        "s.clean_store_name",
        "s.region_title",
        "s.store_sale_sale_date",
        "s.store_sale_revenue",
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

metrics = (
    latest_match
    .select(
        "id",
        "city",
        "clean_store_name",
        "store_sale_sale_date",
        "store_sale_revenue",
        "inventory_quantity",
        F.avg("sqft").over(w3).alias("sqft_avg_2_prior"),
        F.avg("store_sale_revenue").over(w4).alias("store_sale_revenue_avg_5"),
        F.ntile(10).over(w5).alias("sqft_bucket"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.product_id").isin([1, 3, 4, 9])), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "city",
        "store_sale_sale_date",
        "store_sale_revenue",
        "inventory_quantity",
        "sqft_avg_2_prior",
        "store_sale_revenue_avg_5",
        F.when(F.col("city").rlike("^(London|Singapore)$"), "GROUP_A").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("city_zone"),
    )
)
