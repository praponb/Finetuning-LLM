from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age")
w2 = Window.partitionBy("city")

result = (
    df_customers
    .select(
        "id",
        "country",
        "age",
        "city",
        "credit_limit",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 3).alias("credit_limit_share"),
    )
    .orderBy(F.col("age").desc())
)
