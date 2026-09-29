from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("country")

result = (
    df_customers
    .select(
        "id",
        "city",
        "credit_limit",
        "country",
        "age",
        F.min("credit_limit").over(w).alias("city_min_credit_limit"),
        F.min("age").over(w2).alias("country_min_age"),
    )
)
