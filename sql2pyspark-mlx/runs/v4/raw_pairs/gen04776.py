from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "supplier_id")
w2 = Window.orderBy("cost", "id")

valid = (
    df_products
    .filter(F.array_contains("tags", "clearance"))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.trim("category").alias("clean_category"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.rating"), F.col("s.country"))
)

metrics = (
    with_dims
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w)), 4))
    .withColumn("cost_bucket", F.ntile(3).over(w2))
)

result = (
    metrics
    .withColumn("quarter_start", F.date_trunc("QUARTER", "launch_date"))
    .withColumn("activity_month", F.date_trunc("MONTH", "launch_date"))
    .cube("supplier_id", "quarter_start", "activity_month")
    .agg(
        F.size(F.collect_set("category")).alias("num_categories"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("event_count"),
        F.countDistinct("brand").alias("distinct_brands"),
        F.sum(F.when(F.col("country") == "SG", F.col("cost_bucket"))).alias("sg_total_cost_bucket"),
        F.sum("price_share").alias("total_price_share"),
        F.grouping_id().alias("grouping_id"),
    )
)
