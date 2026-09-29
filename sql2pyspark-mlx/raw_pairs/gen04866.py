from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age")
w2 = Window.partitionBy("country")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "age",
        "country",
        F.dense_rank().over(w).alias("age_dense_rank"),
        F.count("*").over(w2).alias("country_count"),
    )
    .orderBy("country")
)
