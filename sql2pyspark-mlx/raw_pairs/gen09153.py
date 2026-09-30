from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("segment", "city").orderBy("credit_limit", "id").rowsBetween(-3, Window.currentRow)
w_recent = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")

filtered = (
    df_customers
    .filter(F.col("is_active") & (~F.col("segment").eqNullSafe(F.lit("SMB"))))
    .withColumn("country_prefix", F.substring("country", 1, 4))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
)

result = (
    filtered
    .filter(F.col("city").isin(["New York", "London"]))
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "country_prefix",
        "is_active_flag",
        F.sum("is_active_flag").over(w_hist).alias("last4_sum_is_active_flag"),
        F.min("is_active_flag").over(w_hist).alias("last4_min_is_active_flag"),
        F.count("is_active_flag").over(w_hist).alias("last4_count_is_active_flag"),
        F.ntile(10).over(w_recent).alias("credit_limit_bucket"),
    )
)
