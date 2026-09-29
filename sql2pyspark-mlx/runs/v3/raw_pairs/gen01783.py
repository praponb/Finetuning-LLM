from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .filter(F.col("signup_date") <= "2025-05-30")
    .select(
        "id",
        "country",
        "city",
        "credit_limit",
        F.dense_rank().over(w).alias("dense_rnk"),
    )
)
