from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_customers
    .filter(F.lower(F.trim("name")) == "grace kim")
    .select(
        "id",
        "country",
        "credit_limit",
        F.sum("credit_limit").over(w).alias("country_sum_credit_limit"),
    )
)
