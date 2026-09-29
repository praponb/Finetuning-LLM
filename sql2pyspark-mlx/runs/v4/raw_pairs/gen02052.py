from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_customers
    .filter(F.col("credit_limit").between(13300, 15400))
    .select(
        "id",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.concat("country", F.lit(", "), "name").alias("country_name"),
        (F.col("credit_limit") + F.col("age")).alias("credit_limit_plus_age"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

result = (
    cleaned
    .withColumn("credit_limit_avg_5", F.avg("credit_limit").over(w))
    .withColumn("cumulative_age", F.sum("age").over(w2))
    .withColumn("country_sum_credit_limit_plus_age", F.sum("credit_limit_plus_age").over(w3))
    .withColumn("cumulative_credit_limit", F.sum("credit_limit").over(w4))
)
