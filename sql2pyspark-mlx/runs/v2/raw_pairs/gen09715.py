from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc())
w2 = Window.partitionBy("city")

result = (
    df_customers
    .filter(F.col("email").like("%@mail.co.th"))
    .select(
        "id",
        "segment",
        "credit_limit",
        "city",
        "age",
        F.rank().over(w).alias("rnk"),
        F.avg("age").over(w2).alias("city_avg_age"),
    )
    .orderBy("segment", F.col("credit_limit").desc())
)
