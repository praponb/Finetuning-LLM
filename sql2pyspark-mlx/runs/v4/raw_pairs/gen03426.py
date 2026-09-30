from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("days_since_sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id").orderBy("sale_date_plus_months", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("store_id").orderBy(F.col("days_since_sale_date").desc_nulls_last())
w4 = Window.partitionBy("store_id").orderBy("sale_date", "id")
w5 = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id")
w6 = Window.partitionBy("store_id").orderBy("revenue", "id")
w_recent = Window.orderBy(F.col("revenue").desc(), "id")
w8 = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("store_id").orderBy("days_since_sale_date_avg_2_prior", "id")
w10 = Window.partitionBy("product_id")

filtered = (
    df_store_sales
    .filter(F.col("units") < 31)
    .select(
        "*",
        F.add_months("sale_date", 6).alias("sale_date_plus_months"),
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

metrics = (
    filtered
    .withColumn("last_known_days_since_sale_date", F.last("days_since_sale_date", ignorenulls=True).over(w))
    .withColumn("days_since_sale_date_avg_2_prior", F.avg("days_since_sale_date").over(w2))
    .withColumn("days_since_sale_date_percent_rank", F.percent_rank().over(w3))
    .withColumn("next_days_since_sale_date", F.lead("days_since_sale_date", 1).over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
)

base = (
    metrics
    .filter(F.col("units") > 38)
    .withColumn("last_known_days_since_sale_date_level", F.when(F.col("last_known_days_since_sale_date") >= 70, "High").when(F.col("last_known_days_since_sale_date") >= 31, "Medium").otherwise("Low"))
)

joined = (
    base
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.store_name"))
)

scored = (
    joined
    .withColumn("prev_revenue", F.lag("revenue", 1).over(w6))
    .withColumn("revenue_bucket", F.ntile(10).over(w_recent))
    .withColumn("first_known_days_since_sale_date_avg_2_prior", F.first("days_since_sale_date_avg_2_prior", ignorenulls=True).over(w8))
    .withColumn("days_since_sale_date_avg_2_prior_bucket", F.ntile(3).over(w_hist))
    .withColumn("product_id_max_revenue", F.max("revenue").over(w10))
)

result = (
    scored
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        "sale_date_plus_months",
        "days_since_sale_date",
        "last_known_days_since_sale_date",
        "days_since_sale_date_avg_2_prior",
        "days_since_sale_date_percent_rank",
    )
    .orderBy(F.col("store_id").desc_nulls_last())
)
