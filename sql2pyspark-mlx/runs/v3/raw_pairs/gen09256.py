from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("age", "id")
w2 = Window.partitionBy("city").orderBy("credit_limit", "id")

result = (
    df_customers
    .filter(F.col("age").isNotNull())
    .select(
        "id",
        "age",
        "city",
        "credit_limit",
        F.ntile(3).over(w).alias("age_ntile"),
        F.row_number().over(w2).alias("credit_limit_row"),
    )
    .orderBy(F.col("id").desc(), F.col("age_ntile").desc())
)
