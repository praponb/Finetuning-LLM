from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")
w2 = Window.partitionBy("city").orderBy("age")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "signup_date",
        "city",
        "age",
        F.ntile(3).over(w).alias("quartile"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
    .orderBy("city")
)
