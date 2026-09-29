from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("signup_date", "id")
w2 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .select(
        "id",
        "city",
        "country",
        "signup_date",
        "age",
        "segment",
        F.lag("age", 1).over(w).alias("prev_age"),
        F.sum("age").over(w2).alias("running_age"),
    )
    .orderBy("signup_date")
)
