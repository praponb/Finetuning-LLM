from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")
w2 = Window.partitionBy("segment", "country")

result = (
    df_customers
    .filter(F.length("name") > 14)
    .select(
        "id",
        "segment",
        "signup_date",
        "country",
        "credit_limit",
        F.row_number().over(w).alias("row_num"),
        F.min("credit_limit").over(w2).alias("segment_min_credit_limit"),
    )
    .orderBy(F.col("country").desc())
)
