from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("credit_limit", "id")
w3 = Window.partitionBy("segment", "city").orderBy("age", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id")
w5 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_customers
    .select(
        "*",
        F.sum("credit_limit").over(w).alias("cumulative_credit_limit"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("age").over(w3).alias("age_avg_6"),
        F.row_number().over(w4).alias("row_seq"),
        F.avg("age").over(w5).alias("age_avg_3"),
    )
)
