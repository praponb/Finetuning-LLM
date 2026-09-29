from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .select(
        "id",
        "city",
        "age",
        "country",
        "credit_limit",
        F.max("age").over(w).alias("city_max_age"),
        F.dense_rank().over(w2).alias("credit_limit_dense_rank"),
    )
)
