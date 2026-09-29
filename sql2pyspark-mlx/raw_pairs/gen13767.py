from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w_recent = Window.partitionBy("product_id").orderBy("revenue", "id")
w4 = Window.partitionBy(F.year("sale_date"), "product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.last("units", ignorenulls=True).over(w).alias("last_known_units"),
        F.ntile(3).over(w2).alias("units_bucket"),
        F.lead("units", 1).over(w_recent).alias("next_units"),
        F.last("revenue", ignorenulls=True).over(w4).alias("last_known_revenue"),
        F.lag("units", 1).over(w_recent).alias("prev_units"),
    )
)

result = (
    enriched
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        "last_known_units",
        "units_bucket",
        "next_units",
        "last_known_revenue",
        "prev_units",
    )
    .orderBy(F.col("units").desc_nulls_first())
)
