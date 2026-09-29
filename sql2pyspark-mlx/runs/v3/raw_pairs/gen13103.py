from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy(F.col("age").desc())
w2 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .select(
        "id",
        "segment",
        "country",
        "age",
        "city",
        "credit_limit",
        F.rank().over(w).alias("rnk"),
        F.dense_rank().over(w2).alias("credit_limit_dense_rank"),
    )
)
