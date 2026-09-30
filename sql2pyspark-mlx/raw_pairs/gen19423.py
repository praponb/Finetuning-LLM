from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_customers
    .select(
        "id",
        "city",
        "country",
        "credit_limit",
        "segment",
        F.last("segment").over(w).alias("last_segment"),
    )
)
