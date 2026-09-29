from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .filter(F.col("city") == "Tokyo")
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.sum("sqft").over(w).alias("running_sqft"),
    )
)
