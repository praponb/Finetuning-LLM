from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_customers
    .select(
        "id",
        "country",
        "credit_limit",
        F.avg("credit_limit").over(w).alias("country_avg_credit_limit"),
    )
)
