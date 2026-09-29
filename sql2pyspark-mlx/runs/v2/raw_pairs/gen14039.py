from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("segment")

result = (
    df_customers
    .filter(F.col("email").like("%@GMAIL.COM"))
    .select(
        "id",
        "country",
        "age",
        "segment",
        "credit_limit",
        F.round((F.col("age") / F.sum("age").over(w)), 3).alias("age_share"),
        F.min("credit_limit").over(w2).alias("segment_min_credit_limit"),
    )
)
