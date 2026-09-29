from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .filter(F.col("opened_date") < "2024-11-01")
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.sum("sqft").over(w).alias("running_sqft"),
    )
)
