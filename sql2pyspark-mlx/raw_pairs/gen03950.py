from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city").orderBy("signup_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("city", "segment").orderBy("credit_limit", "id")
w3 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(-3, -1)

result = (
    df_customers
    .filter(F.col("country").isin(["DE", "SG", "TH"]))
    .select(
        "id",
        "email",
        "country",
        "segment",
        "is_active",
        F.avg("age").over(w).alias("age_avg_5"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("age").over(w3).alias("age_avg_3_prior"),
    )
)
