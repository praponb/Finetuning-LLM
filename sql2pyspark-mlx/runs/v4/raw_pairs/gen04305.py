from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-10-02").cast("date")) & (F.col("status") != "CANCELLED"))
    .select("*", F.add_months("end_date", 6).alias("end_date_plus_months"))
)

result = (
    valid
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.age"), F.col("c.segment"), F.col("c.email"))
)
