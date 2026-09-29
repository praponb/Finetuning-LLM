from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").asc_nulls_last())
w2 = Window.partitionBy("segment").orderBy("credit_limit", "id")
w3 = Window.partitionBy("segment")
w4 = Window.partitionBy("segment", "country").orderBy("credit_limit", "id")
w5 = Window.partitionBy("country")
w6 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city", "country").orderBy(F.col("corr_age_credit_limit").desc())

ranked = (
    df_customers
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .withColumn("credit_limit_bucket", F.ntile(10).over(w2))
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w3))
    .withColumn("credit_limit_bucket_2", F.ntile(3).over(w4))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w5)), 4))
)

metrics = (
    with_windows
    .withColumn("first_known_credit_limit_bucket", F.first("credit_limit_bucket", ignorenulls=True).over(w6))
    .withColumn("corr_age_credit_limit_cume_dist", F.cume_dist().over(w7))
    .withColumn("credit_limit_bucket_share", F.round((F.col("credit_limit_bucket") / F.sum("credit_limit_bucket").over(w5)), 4))
)

result = (
    metrics
    .cube(F.date_trunc("QUARTER", "signup_date").alias("quarter_start"), "segment")
    .agg(
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.bit_and("age").alias("age_bit_and"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.round(F.sum("credit_limit_bucket") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("credit_limit_bucket_per_singapore"),
    )
    .orderBy("credit_limit_bucket_per_singapore")
)
