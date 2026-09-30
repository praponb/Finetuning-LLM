from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id")
w2 = Window.partitionBy("country").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("credit_limit").between(11900, 13600))
    .select(
        "id",
        "country",
        "credit_limit",
        "signup_date",
        F.row_number().over(w).alias("credit_limit_row"),
        F.sum("credit_limit").over(w2).alias("running_credit_limit"),
    )
)
