from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment")

result = (
    df_customers
    .filter(~F.col("country").isin(["TH", "DE", "UK"]))
    .select(
        "id",
        "country",
        "segment",
        "age",
        F.sum("age").over(w).alias("country_sum_age"),
    )
)
