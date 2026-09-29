from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy("age", "id")

result = (
    df_customers
    .filter(~F.col("country").isin(["TH", "SG"]))
    .select(
        "id",
        "segment",
        "country",
        "age",
        "credit_limit",
        F.first("credit_limit").over(w).alias("first_credit_limit"),
    )
    .orderBy(F.col("credit_limit").desc())
)
