from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .filter(F.col("units").isNull())
    .select(
        "id",
        "sale_date",
        "units",
        "product_id",
        F.sum("units").over(w).alias("running_units"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("id").desc())
)
