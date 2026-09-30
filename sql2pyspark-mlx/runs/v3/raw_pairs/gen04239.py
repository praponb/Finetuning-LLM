from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .filter(F.col("name").like("%Chen%"))
    .select(
        "id",
        "country",
        "signup_date",
        "credit_limit",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .select("id", "country", "signup_date", "credit_limit")
)
