from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc())
w2 = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select(
        "id",
        "segment",
        "signup_date",
        "age",
        F.rank().over(w).alias("rnk"),
        F.sum("age").over(w2).alias("running_age"),
    )
)
