from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc())
w2 = Window.orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("name").like("b%"))
    .select(
        "id",
        "segment",
        "age",
        "signup_date",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.lead("age", 1).over(w2).alias("next_age"),
    )
    .orderBy(F.col("id").desc())
)
