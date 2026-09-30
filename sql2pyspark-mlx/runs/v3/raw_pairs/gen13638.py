from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment")

result = (
    df_customers
    .select(
        "id",
        "country",
        "segment",
        "age",
        F.sum("age").over(w).alias("country_sum_age"),
    )
    .orderBy(F.col("id").desc())
)
