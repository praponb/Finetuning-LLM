from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")

ranked = (
    df_customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .groupBy(F.when(F.col("age") >= 55, "HIGH").otherwise("LOW").alias("age_tier"))
    .agg(
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
    )
)
