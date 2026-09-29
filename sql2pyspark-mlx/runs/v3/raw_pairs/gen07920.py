from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_customers
    .filter(F.col("email").like("%@startup.io"))
    .select(
        "id",
        "country",
        "credit_limit",
        F.sum("credit_limit").over(w).alias("country_sum_credit_limit"),
    )
    .orderBy(F.col("country_sum_credit_limit").desc())
)
