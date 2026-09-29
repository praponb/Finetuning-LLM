from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3]))
    .select(
        "id",
        "store_id",
        "units",
        F.round((F.col("units") / F.sum("units").over(w)), 3).alias("units_share"),
    )
    .orderBy(F.col("units_share").desc())
)
