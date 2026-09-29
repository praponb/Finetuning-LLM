from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("segment")

result = (
    df_customers
    .filter(F.col("credit_limit") <= 12900)
    .select(
        "id",
        "country",
        "age",
        "segment",
        F.max("age").over(w).alias("country_max_age"),
        F.count("*").over(w2).alias("segment_count"),
    )
    .orderBy("segment_count")
)
