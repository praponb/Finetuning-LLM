from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")
w2 = Window.partitionBy(F.year("signup_date"), "city").orderBy("signup_date")

result = (
    df_customers
    .filter(F.col("signup_date").between("2024-10-01", "2025-09-08"))
    .select(
        "id",
        "country",
        "signup_date",
        "credit_limit",
        "city",
        F.lead("credit_limit", 1).over(w).alias("next_credit_limit"),
        F.dense_rank().over(w2).alias("signup_date_dense_rank"),
    )
)
