from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_customers
    .select(
        "id",
        "city",
        "age",
        "country",
        F.last("country").over(w).alias("last_country"),
    )
)
