from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-05-25")))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "credit_limit",
        "age",
        F.trim("name").alias("clean_name"),
    )
)

result = (
    base
    .groupBy("country")
    .pivot("city", ["Singapore", "Tokyo"])
    .agg(F.sum("credit_limit").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
