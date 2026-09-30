from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .select(
        "id",
        "sqft",
        "opened_date",
        "region",
        F.last("opened_date").over(w).alias("last_opened_date"),
        F.sum("sqft").over(w2).alias("running_sqft"),
    )
)
