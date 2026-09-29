from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit")
w2 = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "country",
        "credit_limit",
        "segment",
        "signup_date",
        "age",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.lead("age", 1, 0).over(w2).alias("next_age"),
    )
    .orderBy(F.col("id").desc(), F.col("credit_limit").desc())
)
