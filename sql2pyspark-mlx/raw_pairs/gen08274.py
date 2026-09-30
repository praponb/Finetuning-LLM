from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("product_category").orderBy(F.col("product_cost").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w5 = Window.orderBy(F.col("country_avg_product_cost").desc())
w6 = Window.partitionBy("country").orderBy(F.col("product_cost_bucket").desc(), "id")

with_latest = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.cost").alias("product_cost"),
        F.col("c.category").alias("product_category"),
    )
)

metrics = (
    with_latest
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "product_category",
        F.avg("product_cost").over(w2).alias("country_avg_product_cost"),
        F.ntile(10).over(w3).alias("product_cost_bucket"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.rating",
        "s.country_avg_product_cost",
        "s.product_cost_bucket",
        F.col("c.category").alias("product_category_2"),
    )
)

scored = (
    with_last
    .filter(F.col("product_category_2") != "Home")
    .select(
        "id",
        "country",
        "product_cost_bucket",
        F.cume_dist().over(w5).alias("country_avg_product_cost_cume_dist"),
        F.ntile(5).over(w6).alias("product_cost_bucket_bucket"),
    )
)

totals = (
    scored
    .groupBy(
        "country",
        F.when(F.col("product_cost_bucket_bucket") >= 9, "HIGH").otherwise("LOW").alias("product_cost_bucket_bucket_tier"),
        F.when(F.col("country_avg_product_cost_cume_dist") >= 0.64, "HIGH").otherwise("LOW").alias("country_avg_product_cost_cume_dist_tier"),
    )
    .agg(
        F.min("product_cost_bucket").alias("min_product_cost_bucket"),
        F.avg(F.when(F.col("country") == "US", F.col("country_avg_product_cost_cume_dist"))).alias("us_avg_country_avg_product_cost_cume_dist"),
    )
)

result = (
    totals
    .select(
        "country",
        "product_cost_bucket_bucket_tier",
        "country_avg_product_cost_cume_dist_tier",
    )
)
