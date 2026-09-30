from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_customers
    .select(
        "id",
        "country",
        "credit_limit",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 3).alias("credit_limit_share"),
    )
    .orderBy("country")
)
