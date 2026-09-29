from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit")

result = (
    df_customers
    .select(
        "id",
        "city",
        "credit_limit",
        "country",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 2)
    .select("id", "city", "credit_limit", "country")
    .orderBy(F.col("id").desc(), "country")
)
