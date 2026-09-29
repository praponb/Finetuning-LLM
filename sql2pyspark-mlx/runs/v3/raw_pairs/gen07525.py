from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")

result = (
    df_customers
    .filter(F.col("credit_limit") < 15600)
    .select(
        "id",
        "country",
        "city",
        "age",
        F.sum("age").over(w).alias("country_sum_age"),
    )
)
