from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .select(
        "id",
        "city",
        "signup_date",
        "credit_limit",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .orderBy("city")
)
