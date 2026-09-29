from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")
w2 = Window.partitionBy("segment").orderBy("age")

result = (
    df_customers
    .select(
        "id",
        "country",
        "signup_date",
        "age",
        "segment",
        (F.col("age") - F.lag("age", 1, 0).over(w)).alias("age_change"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
