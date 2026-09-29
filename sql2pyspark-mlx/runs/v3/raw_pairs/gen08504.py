from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit")

result = (
    df_customers
    .filter(F.col("credit_limit") >= 12300)
    .select(
        "id",
        "city",
        "credit_limit",
        F.percent_rank().over(w).alias("credit_limit_pct_rank"),
    )
    .orderBy(F.col("city").desc())
)
