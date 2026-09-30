from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .filter(F.year("signup_date") >= 2025)
    .select(
        "id",
        "city",
        "segment",
        "signup_date",
        "age",
        F.sum("age").over(w).alias("running_age"),
    )
)
