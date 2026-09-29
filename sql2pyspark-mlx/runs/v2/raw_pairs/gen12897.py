from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("city").isin(["Tokyo", "Singapore", "New York"]))
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "region",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.lag("sqft", 1).over(w2).alias("prev_sqft"),
    )
    .orderBy(F.col("id").desc())
)
