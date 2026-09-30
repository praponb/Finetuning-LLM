from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_last())
w2 = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_first())

result = (
    df_customers
    .filter(F.col("country").isin(["SG", "JP"]))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.dense_rank().over(w).alias("age_dense_rank"),
        F.rank().over(w2).alias("age_rank"),
    )
)
