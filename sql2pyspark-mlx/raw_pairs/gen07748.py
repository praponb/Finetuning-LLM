from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        "customer_id",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 1)
)
