from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_stores
    .select(
        "id",
        "city",
        "sqft",
        "opened_date",
        F.last("opened_date").over(w).alias("last_opened_date"),
    )
)
