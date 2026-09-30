from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())

cleaned = (
    df_customers
    .filter(~F.col("name").like("%Olivia%"))
    .select(
        "id",
        "name",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.trim("name").alias("clean_name"),
        F.initcap(F.trim("city")).alias("city_title"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.is_active",
        "s.clean_name",
        "s.city_title",
        F.col("c.status").alias("subscription_status"),
    )
)
