from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id")
w2 = Window.partitionBy("segment")

result = (
    df_customers
    .select(
        "id",
        "city",
        "age",
        "segment",
        "credit_limit",
        F.ntile(2).over(w).alias("quartile"),
        F.avg("credit_limit").over(w2).alias("segment_avg_credit_limit"),
    )
)
