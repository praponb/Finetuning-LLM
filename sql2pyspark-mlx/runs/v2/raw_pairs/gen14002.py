from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_customers
    .select(
        "id",
        "city",
        "credit_limit",
        F.min("credit_limit").over(w).alias("city_min_credit_limit"),
    )
)
