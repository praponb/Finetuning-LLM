from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "sale_date"), "store_id")

result = (
    df_store_sales
    .filter(F.col("units") == 23)
    .select(
        "id",
        "sale_date",
        "store_id",
        "units",
        F.min("units").over(w).alias("sale_date_period_min_units"),
    )
    .orderBy(F.col("sale_date").desc())
)
