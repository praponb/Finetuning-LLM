from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.partitionBy("country")
w3 = Window.orderBy("age", "id")
w4 = Window.orderBy("is_active_flag", "id")

filtered = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2024-01-19").cast("date"))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.sum("age").over(w).alias("segment_sum_age"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("event_seq_2"),
    )
)

valid = (
    with_windows
    .filter(F.col("segment_sum_age") < 249)
    .withColumn("days_since_signup_date", F.datediff(F.current_date(), "signup_date"))
    .withColumn("segment_sum_age_band", F.when(F.col("segment_sum_age") >= 337, "High").when(F.col("segment_sum_age") >= 91, "Medium").otherwise("Low"))
)

result = (
    valid
    .select(
        "id",
        "email",
        "city",
        "is_active",
        "credit_limit",
        "age",
        "is_active_flag",
        "segment_sum_age",
        "event_seq",
    )
    .orderBy("id")
    .limit(10)
)
