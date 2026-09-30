from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .filter(F.col("signup_date") <= "2025-09-05")
    .select(
        "id",
        "country",
        "signup_date",
        F.dense_rank().over(w).alias("signup_date_dense_rank"),
    )
    .orderBy("id")
)
