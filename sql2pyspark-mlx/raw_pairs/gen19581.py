from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "sale_date"), "product_id").orderBy(F.col("units").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "product_id",
        "units",
        "store_id",
        F.last("store_id").over(w).alias("last_store_id"),
    )
)
