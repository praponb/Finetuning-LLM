from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date")
w2 = Window.partitionBy("segment").orderBy("signup_date")

result = (
    df_customers
    .filter(F.col("name").isNull())
    .select(
        "id",
        "city",
        "signup_date",
        "segment",
        F.rank().over(w).alias("signup_date_rank"),
        F.rank().over(w2).alias("rnk"),
    )
)
